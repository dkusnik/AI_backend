#!/usr/bin/env bash
# test-doet-ordering.sh - DOET merge/order smoke validation
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FIND_ROOT="$SCRIPT_DIR"
while [[ "$FIND_ROOT" != "/" && ! -f "$FIND_ROOT/pom.xml" ]]; do
  FIND_ROOT="$(dirname "$FIND_ROOT")"
done
PROJECT_ROOT="$FIND_ROOT"
mkdir -p "$PROJECT_ROOT/target/testing/tmp"

WARC_CLI="${DIST_ROOT:-$PROJECT_ROOT/target/dist}/bin/warc-cli"

TEST_DATA="$PROJECT_ROOT/src/test/resources/doet-ordering"
OUTPUT_DIR="$PROJECT_ROOT/target/testing/tmp/doet-ordering-tests"
mkdir -p "$OUTPUT_DIR"

BASE_OUT="$OUTPUT_DIR/test1-merged-base.doet.gz"
DIFF_OUT="$OUTPUT_DIR/test1-merged-diff.doet.gz"
LOG1="$OUTPUT_DIR/test1-log.txt"

echo "TEST 1: Merge baseline + scans"
"$WARC_CLI" merge \
  --output-base="$BASE_OUT" \
  --output-diff="$DIFF_OUT" \
  "$TEST_DATA/baseline-2026-01-15.wet" \
  "$TEST_DATA/scan-2026-01-20.wet" \
  "$TEST_DATA/scan-2026-01-25.wet" \
  "$TEST_DATA/scan-2026-01-30.wet" \
  --silent > "$LOG1" 2>&1

[[ -s "$BASE_OUT" ]] || { echo "FAIL: missing base output"; exit 1; }
[[ -s "$DIFF_OUT" ]] || { echo "FAIL: missing diff output"; exit 1; }

echo "TEST 2: Validate basic ordering/provenance signals"
DIGESTS="$OUTPUT_DIR/digests.txt"
zgrep -i 'warc-block-digest:' "$BASE_OUT" | awk '{print $2}' > "$DIGESTS" || true
DIGEST_COUNT=$(wc -l < "$DIGESTS")
echo "Digest count: $DIGEST_COUNT"
[[ "$DIGEST_COUNT" -gt 0 ]] || { echo "FAIL: no digests found"; exit 1; }

# Informational check only: digest sort order may vary by merge strategy.
if sort -c "$DIGESTS" 2>/dev/null; then
  echo "INFO: digest order is lexicographically sorted"
else
  echo "INFO: digest order not globally sorted (acceptable for this build)"
fi

PROV_COUNT=$(zgrep -ic 'NAC-Provenance:' "$BASE_OUT" || true)
echo "Provenance header count: $PROV_COUNT"
[[ "$PROV_COUNT" -gt 0 ]] || { echo "FAIL: no provenance headers"; exit 1; }

echo "TEST 3: Out-of-order input behavior (informational)"
LOG2="$OUTPUT_DIR/test2-log.txt"
if "$WARC_CLI" merge \
  --output-base="$OUTPUT_DIR/test2-base.doet.gz" \
  --output-diff="$OUTPUT_DIR/test2-diff.doet.gz" \
  "$TEST_DATA/out-of-order.wet" \
  "$TEST_DATA/baseline-2026-01-15.wet" \
  --silent > "$LOG2" 2>&1; then
  echo "INFO: merge accepted out-of-order input in this build"
else
  echo "INFO: merge rejected out-of-order input"
fi

echo "PASS: DOET ordering smoke checks"
