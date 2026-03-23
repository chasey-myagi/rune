#!/usr/bin/env bash
#
# File Upload Example — multipart upload + download via Rune Gate.
#
# Prerequisites:
#   1. Start the Rune server:   cargo run -p rune-server
#   2. Start the Python caster: python examples/python-caster/main.py
#      (provides the "file_info" rune with gate path /file-info)
#
# Environment:
#   RUNE_ADDR  — HTTP base URL (default: http://127.0.0.1:50060)

set -euo pipefail

RUNE_ADDR="${RUNE_ADDR:-http://127.0.0.1:50060}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
SAMPLE_FILE="$SCRIPT_DIR/sample.txt"

echo "=== File Upload Example ==="
echo "Runtime: $RUNE_ADDR"
echo ""

# -----------------------------------------------------------------------
# Step 1: Upload a single file via multipart/form-data
# -----------------------------------------------------------------------
echo "--- Uploading sample.txt to file_info rune ---"
RESPONSE=$(curl -s -X POST "$RUNE_ADDR/file-info" \
  -F "input={\"tag\":\"demo\"};type=application/json" \
  -F "file=@$SAMPLE_FILE;type=text/plain")
echo "$RESPONSE" | python3 -m json.tool || echo "$RESPONSE"
echo ""

# -----------------------------------------------------------------------
# Step 2: Upload multiple files
# -----------------------------------------------------------------------
echo "--- Uploading multiple files ---"

# Create a temporary second file
TEMP_FILE=$(mktemp /tmp/rune-upload-XXXXXX.json)
echo '{"key": "value", "numbers": [1, 2, 3]}' > "$TEMP_FILE"

RESPONSE=$(curl -s -X POST "$RUNE_ADDR/file-info" \
  -F "input={\"tag\":\"multi\"};type=application/json" \
  -F "file1=@$SAMPLE_FILE;type=text/plain" \
  -F "file2=@$TEMP_FILE;type=application/json")
echo "$RESPONSE" | python3 -m json.tool || echo "$RESPONSE"
echo ""

rm -f "$TEMP_FILE"

# -----------------------------------------------------------------------
# Step 3: Upload via the debug API (by rune name)
# -----------------------------------------------------------------------
echo "--- Upload via debug API ---"
RESPONSE=$(curl -s -X POST "$RUNE_ADDR/api/v1/runes/file_info/run" \
  -F "input={\"tag\":\"debug-api\"};type=application/json" \
  -F "doc=@$SAMPLE_FILE;type=text/plain")
echo "$RESPONSE" | python3 -m json.tool || echo "$RESPONSE"
echo ""

# Try to extract a file_id for download
FILE_ID=$(echo "$RESPONSE" | python3 -c "
import sys, json
data = json.load(sys.stdin)
files = data.get('files', [])
if files:
    print(files[0].get('file_id', ''))
" 2>/dev/null || true)

# -----------------------------------------------------------------------
# Step 4: Download a file (if file_id was returned)
# -----------------------------------------------------------------------
if [ -n "$FILE_ID" ]; then
  echo "--- Downloading file: $FILE_ID ---"
  curl -s -o /tmp/rune-downloaded.txt "$RUNE_ADDR/api/v1/files/$FILE_ID"
  echo "Downloaded to /tmp/rune-downloaded.txt"
  echo "Content:"
  cat /tmp/rune-downloaded.txt
  echo ""
  rm -f /tmp/rune-downloaded.txt
else
  echo "--- Skipping download (no file_id in response) ---"
fi
echo ""

echo "=== Done ==="
