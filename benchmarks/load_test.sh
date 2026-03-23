#!/bin/bash
# Rune HTTP Load Test
# Prerequisites: rune-server --dev is running, `hey` is installed
# Install hey: brew install hey / go install github.com/rakyll/hey@latest

set -euo pipefail

BASE_URL=${RUNE_ADDR:-http://localhost:50060}
DURATION=${DURATION:-10}   # seconds
CONCURRENCY=${CONCURRENCY:-10}

echo "=== Rune HTTP Load Test ==="
echo "Target:      $BASE_URL"
echo "Duration:    ${DURATION}s"
echo "Concurrency: $CONCURRENCY"
echo "Date:        $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo ""

# Check if hey is available
if ! command -v hey &> /dev/null; then
    echo "ERROR: 'hey' is not installed."
    echo "Install with: brew install hey"
    exit 1
fi

# Check if server is reachable
if ! curl -sf "$BASE_URL/health" > /dev/null 2>&1; then
    echo "ERROR: Server not reachable at $BASE_URL"
    echo "Start with: cargo run -p rune-server -- --dev"
    exit 1
fi

echo "--- 1. Health Check (baseline) ---"
hey -z "${DURATION}s" -c "$CONCURRENCY" "$BASE_URL/health" 2>&1 \
    | grep -E "Requests/sec|Average|Fastest|Slowest|99%" || true
echo ""

echo "--- 2. List Runes ---"
hey -z "${DURATION}s" -c "$CONCURRENCY" "$BASE_URL/api/v1/runes" 2>&1 \
    | grep -E "Requests/sec|Average|Fastest|Slowest|99%" || true
echo ""

echo "--- 3. Echo Sync Call ---"
hey -z "${DURATION}s" -c "$CONCURRENCY" \
    -m POST \
    -d '{"msg":"hello"}' \
    -T "application/json" \
    "$BASE_URL/api/v1/runes/hello/run" 2>&1 \
    | grep -E "Requests/sec|Average|Fastest|Slowest|99%" || true
echo ""

echo "--- 4. Stats API ---"
hey -z "${DURATION}s" -c "$CONCURRENCY" "$BASE_URL/api/v1/stats" 2>&1 \
    | grep -E "Requests/sec|Average|Fastest|Slowest|99%" || true
echo ""

echo "--- 5. Flow Sync Run ---"
hey -z "${DURATION}s" -c "$CONCURRENCY" \
    -m POST \
    -d '{"input":"test"}' \
    -T "application/json" \
    "$BASE_URL/api/v1/flows/pipeline/run" 2>&1 \
    | grep -E "Requests/sec|Average|Fastest|Slowest|99%" || true
echo ""

echo "=== Load Test Complete ==="
