#!/bin/bash
# repro_merge_single.sh - Verify single-file merge fix (Task #69)
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FIND_ROOT="$SCRIPT_DIR"
while [[ "$FIND_ROOT" != "/" && ! -f "$FIND_ROOT/pom.xml" ]]; do
  FIND_ROOT="$(dirname "$FIND_ROOT")"
done
PROJECT_ROOT="$FIND_ROOT"
WARC_CLI="$SCRIPT_DIR/../bin/warc-cli"
TMP_DIR="$PROJECT_ROOT/target/testing/tmp/repro_merge_single_$$"
TEST_DATA="${LOCAL_TEST_DIR:-$PROJECT_ROOT/shared}/tiny.warc.gz"

mkdir -p "$TMP_DIR"
cd "$TMP_DIR"

echo "=== Single-File Merge Verification (Task #69) ==="
echo "Input: $TEST_DATA"
echo ""

# Run merge with single file
"$WARC_CLI" merge \
    --output-base=base.doet.gz \
    --output-diff=diff.doet.gz \
    "$TEST_DATA"

# Count records
BASE_COUNT=$(zcat base.doet.gz 2>/dev/null | grep -c "^WARC-Type:" || echo 0)
DIFF_COUNT=$(zcat diff.doet.gz 2>/dev/null | grep -c "^WARC-Type:" || echo 0)

echo "Base output: $BASE_COUNT records"
echo "Diff output: $DIFF_COUNT records"
echo ""

if [[ $BASE_COUNT -gt 0 ]] && [[ $DIFF_COUNT -gt 0 ]]; then
    echo "✓ PASS: Both outputs have records (expected behavior)"
    exit 0
else
    echo "✗ FAIL: Expected records in both outputs"
    echo "  Base: $BASE_COUNT (expected > 0)"
    echo "  Diff: $DIFF_COUNT (expected > 0)"
    exit 1
fi
