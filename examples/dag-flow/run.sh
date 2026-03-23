#!/usr/bin/env bash
#
# DAG Flow Example — register and execute a 3-layer pipeline.
#
# Prerequisites:
#   1. Start the Rune server:   cargo run -p rune-server
#   2. Start casters that provide: extract, analyze, translate, merge runes
#      (e.g. python examples/python-caster/main.py — or register your own)
#
# Environment:
#   RUNE_ADDR  — HTTP base URL (default: http://127.0.0.1:50060)
#   RUNE_KEY   — API key (optional, for authenticated servers)

set -euo pipefail

RUNE_ADDR="${RUNE_ADDR:-http://127.0.0.1:50060}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Header for authenticated requests
AUTH_HEADER=""
if [ -n "${RUNE_KEY:-}" ]; then
  AUTH_HEADER="-H \"Authorization: Bearer $RUNE_KEY\""
fi

echo "=== DAG Flow Example ==="
echo "Runtime: $RUNE_ADDR"
echo ""

# -----------------------------------------------------------------------
# Step 1: Register the flow
# -----------------------------------------------------------------------
echo "--- Registering flow from pipeline.json ---"
curl -s -X POST "$RUNE_ADDR/api/v1/flows" \
  -H "Content-Type: application/json" \
  -d @"$SCRIPT_DIR/pipeline.json" | python3 -m json.tool || true
echo ""

# -----------------------------------------------------------------------
# Step 2: List flows
# -----------------------------------------------------------------------
echo "--- Listing registered flows ---"
curl -s "$RUNE_ADDR/api/v1/flows" | python3 -m json.tool || true
echo ""

# -----------------------------------------------------------------------
# Step 3: Get flow detail
# -----------------------------------------------------------------------
echo "--- Flow detail: etl-pipeline ---"
curl -s "$RUNE_ADDR/api/v1/flows/etl-pipeline" | python3 -m json.tool || true
echo ""

# -----------------------------------------------------------------------
# Step 4: Execute the flow (sync)
# -----------------------------------------------------------------------
echo "--- Executing flow (sync) ---"
curl -s -X POST "$RUNE_ADDR/api/v1/flows/etl-pipeline/run" \
  -H "Content-Type: application/json" \
  -d '{"text": "hello world", "target_lang": "zh"}' | python3 -m json.tool || true
echo ""

# -----------------------------------------------------------------------
# Step 5: Execute the flow (async)
# -----------------------------------------------------------------------
echo "--- Executing flow (async) ---"
RESPONSE=$(curl -s -X POST "$RUNE_ADDR/api/v1/flows/etl-pipeline/run?async=true" \
  -H "Content-Type: application/json" \
  -d '{"text": "hello world", "target_lang": "zh"}')
echo "$RESPONSE" | python3 -m json.tool || true

TASK_ID=$(echo "$RESPONSE" | python3 -c "import sys,json; print(json.load(sys.stdin).get('task_id',''))" 2>/dev/null || true)
if [ -n "$TASK_ID" ]; then
  echo ""
  echo "--- Polling task: $TASK_ID ---"
  sleep 2
  curl -s "$RUNE_ADDR/api/v1/tasks/$TASK_ID" | python3 -m json.tool || true
fi
echo ""

# -----------------------------------------------------------------------
# Step 6: Clean up — delete the flow
# -----------------------------------------------------------------------
echo "--- Deleting flow ---"
curl -s -X DELETE "$RUNE_ADDR/api/v1/flows/etl-pipeline" -o /dev/null -w "HTTP %{http_code}\n"
echo ""

echo "=== Done ==="
