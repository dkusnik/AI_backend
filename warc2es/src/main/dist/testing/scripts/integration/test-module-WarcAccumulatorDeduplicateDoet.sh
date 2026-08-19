#!/bin/bash
# test-module-WarcAccumulatorDeduplicateDoet.sh - DOET accumulator module tests
# Category: INTEGRATION (runs via target/dist/bin)
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT_NAME="$(basename "${BASH_SOURCE[0]}" .sh)"

# === Paths ===
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../../.." && pwd)"
BIN_DIR="${DIST_ROOT:-$PROJECT_ROOT/target/dist}/bin"
TMP_DIR="$PROJECT_ROOT/target/testing/tmp"
REPORTS_DIR="$PROJECT_ROOT/target/testing/tmp/reports"

# === Test source files ===
LOCAL_TEST_DIR="${LOCAL_TEST_DIR:-$HOME/.local/test}"
SAMPLE_WARC="$LOCAL_TEST_DIR/example.com.warc.gz"

# === Output ===
TIMESTAMP=$(date +%Y%m%d-%H%M%S)
OUTPUT_DIR="$TMP_DIR/doet-module-$TIMESTAMP"
LOG_DIR="$OUTPUT_DIR/logs"
REPORT_FILE="$REPORTS_DIR/${TIMESTAMP}_${SCRIPT_NAME}.report.md"

# === Binary ===
WARC="$BIN_DIR/warc-cli"

echo "=== WarcAccumulatorDeduplicateDoet Module Tests ==="
echo ""

mkdir -p "$OUTPUT_DIR" "$LOG_DIR" "$REPORTS_DIR"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

PASSED=0
FAILED=0
TOTAL=0

print_header() {
    echo ""
    echo "============================================================"
    echo "$1"
    echo "============================================================"
}

# ============================================================
# TEST 1: Module Configuration
# ============================================================
print_header "TEST 1: Module loads and configures"
TOTAL=$((TOTAL + 1))

# Create small test WET with duplicate content
TEST_WET="$OUTPUT_DIR/test-input.wet.gz"
TEST_DOET="$OUTPUT_DIR/test-output.doet.gz"

# Generate test WET from example WARC
if [ -f "$SAMPLE_WARC" ]; then
    "$WARC" extract-text "$TEST_WET" "$SAMPLE_WARC" 2>&1 | tail -3

    if [ -f "$TEST_WET" ]; then
        echo -e "  ${GREEN}PASS${NC}: Test WET generated"
        PASSED=$((PASSED + 1))
    else
        echo -e "  ${RED}FAIL${NC}: Could not generate test WET"
        FAILED=$((FAILED + 1))
    fi
else
    echo -e "  ${YELLOW}SKIP${NC}: No sample WARC available"
    exit 0
fi

# ============================================================
# TEST 2: Deduplication
# ============================================================
print_header "TEST 2: Deduplication with RocksDB"
TOTAL=$((TOTAL + 1))

# Clean previous RocksDB
rm -rf "$OUTPUT_DIR/db"

"$WARC" wet2doet "$TEST_DOET" "$TEST_WET" \
    --processor.doet-accumulator.rocksdb-path="$OUTPUT_DIR/db" \
    2>&1 | tee "$LOG_DIR/dedup.log" | tail -10

# Check RocksDB was created
if [ -d "$OUTPUT_DIR/db" ]; then
    echo -e "  ${GREEN}PASS${NC}: RocksDB created at $OUTPUT_DIR/db"
    PASSED=$((PASSED + 1))
else
    echo -e "  ${RED}FAIL${NC}: RocksDB not created"
    FAILED=$((FAILED + 1))
fi

# ============================================================
# TEST 3: Output file verification
# ============================================================
print_header "TEST 3: Output file verification"
TOTAL=$((TOTAL + 1))

if [ -f "$TEST_DOET" ]; then
    SIZE=$(stat -c %s "$TEST_DOET")
    RECORDS=$(zcat "$TEST_DOET" 2>/dev/null | grep -c "^WARC/1" || echo "0")
    echo "  Output size: $SIZE bytes"
    echo "  Output records: $RECORDS"

    if [ "$RECORDS" -gt 0 ]; then
        echo -e "  ${GREEN}PASS${NC}: DOET output contains records"
        PASSED=$((PASSED + 1))
    else
        echo -e "  ${RED}FAIL${NC}: DOET output empty"
        FAILED=$((FAILED + 1))
    fi
else
    echo -e "  ${RED}FAIL${NC}: No output file"
    FAILED=$((FAILED + 1))
fi

# ============================================================
# TEST 4: Metrics check
# ============================================================
print_header "TEST 4: Metrics verification"
TOTAL=$((TOTAL + 1))

if grep -q "doet-accumulator" "$LOG_DIR/dedup.log" 2>/dev/null || \
   grep -q "unique\|duplicates" "$LOG_DIR/dedup.log" 2>/dev/null; then
    echo -e "  ${GREEN}PASS${NC}: Module metrics logged"
    PASSED=$((PASSED + 1))
else
    echo -e "  ${YELLOW}WARN${NC}: Module metrics not found in log"
    FAILED=$((FAILED + 1))
fi

# ============================================================
# SUMMARY
# ============================================================
print_header "Test Summary"
echo "Total: $TOTAL"
echo -e "Passed: ${GREEN}$PASSED${NC}"
echo -e "Failed: ${RED}$FAILED${NC}"
echo ""
echo "Artifacts: $OUTPUT_DIR"
echo "Logs: $LOG_DIR"

if [ "$FAILED" -gt 0 ]; then
    echo -e "${RED}SOME TESTS FAILED${NC}"
    exit 1
else
    echo -e "${GREEN}ALL TESTS PASSED${NC}"
    exit 0
fi
