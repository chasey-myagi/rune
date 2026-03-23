#!/bin/bash
# Rune Performance Benchmark Suite — Run All
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
RESULTS_DIR="$SCRIPT_DIR/results"

mkdir -p "$RESULTS_DIR"

echo "============================================"
echo "  Rune Performance Benchmark Suite"
echo "  $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "============================================"
echo ""

# --- Step 1: Micro-benchmarks ---
echo "[1/3] Running micro-benchmarks (criterion)..."
echo "      This may take a few minutes."
echo ""

cd "$ROOT_DIR"
cargo bench --bench core_benchmarks 2>&1 | tee "$RESULTS_DIR/micro.txt"

echo ""
echo "[1/3] Micro-benchmarks complete."
echo "      HTML reports: target/criterion/report/index.html"
echo ""

# --- Step 2: Load tests (optional — requires running server) ---
BASE_URL=${RUNE_ADDR:-http://localhost:50060}

if curl -sf "$BASE_URL/health" > /dev/null 2>&1; then
    echo "[2/3] Running HTTP load tests..."
    echo ""
    "$SCRIPT_DIR/load_test.sh" 2>&1 | tee "$RESULTS_DIR/load.txt"
    echo ""
    echo "[2/3] Load tests complete."
else
    echo "[2/3] Skipping load tests (server not running at $BASE_URL)."
    echo "      Start with: cargo run -p rune-server -- --dev"
    echo ""
fi

# --- Step 3: Generate report ---
echo "[3/3] Generating performance report..."
echo ""

python3 "$SCRIPT_DIR/report.py" > "$RESULTS_DIR/report.md"

echo "[3/3] Report generated."
echo ""
echo "============================================"
echo "  Results"
echo "============================================"
echo "  Micro-benchmarks: $RESULTS_DIR/micro.txt"
echo "  Load tests:       $RESULTS_DIR/load.txt"
echo "  Report:           $RESULTS_DIR/report.md"
echo "  Criterion HTML:   $ROOT_DIR/target/criterion/report/index.html"
echo "============================================"
