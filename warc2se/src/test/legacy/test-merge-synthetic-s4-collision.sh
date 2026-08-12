#!/usr/bin/env bash
# Test: Digest collision - same digest, different payload
# Expected behavior: Both records emitted with "new-content" provenance

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."

# Setup
TEST_NAME="merge-synthetic-s4"
OUTPUT_DIR="/tmp/warc-test-output-$$/${TEST_NAME}"
mkdir -p "$OUTPUT_DIR"

INPUT_F1="${OUTPUT_DIR}/s4-crawl1.wet"
INPUT_F2="${OUTPUT_DIR}/s4-crawl2.wet"
OUTPUT_BASE="${OUTPUT_DIR}/s4-merged-base.wet"
OUTPUT_DIFF="${OUTPUT_DIR}/s4-merged-diff.wet"

# Generate F1 (crawl 1)
cat > "$INPUT_F1" << 'EOF'
WARC/1.0
content-type: text/plain; charset=utf-8
warc-payload-digest: xxh128:1234567890abcdef
warc-target-uri: http://example.com/page1
warc-date: 2026-01-01T10:00:00Z
warc-type: conversion
content-length: 28

Payload A - original content

EOF

# Generate F2 (crawl 2) - SAME DIGEST, DIFFERENT PAYLOAD
cat > "$INPUT_F2" << 'EOF'
WARC/1.0
content-type: text/plain; charset=utf-8
warc-payload-digest: xxh128:1234567890abcdef
warc-target-uri: http://example.com/page1
warc-date: 2026-02-01T10:00:00Z
warc-type: conversion
content-length: 28

Payload B - DIFFERENT CONTENT

EOF

# Compress inputs (DOET merge requires .gz)
gzip -f "$INPUT_F1"
gzip -f "$INPUT_F2"
INPUT_F1="${INPUT_F1}.gz"
INPUT_F2="${INPUT_F2}.gz"

# Run merge
echo "Running merge with digest collision..."
bin/warc-cli merge \
  --input.files="$INPUT_F1" \
  --processor.doet-accumulator.secondary-input="$INPUT_F2" \
  --processor.doet-accumulator.primary-file-pattern="s4-crawl1" \
  --processor.doet-accumulator.baseline-date=2026-01-01T10:00:00Z \
  --processor.doet-accumulator.mode=incremental \
  --output.file="$OUTPUT_BASE" \
  --consumer.warc-base.diff-output="$OUTPUT_DIFF" \
  --output.format=WARC_WET \
  --global.verbose=false

# Decompress outputs
gunzip -f "$OUTPUT_BASE"
gunzip -f "$OUTPUT_DIFF"
OUTPUT_BASE="${OUTPUT_BASE%.gz}"
OUTPUT_DIFF="${OUTPUT_DIFF%.gz}"

# Analyze results
echo ""
echo "=== BASE OUTPUT ==="
cat "$OUTPUT_BASE"
echo ""
echo "=== DIFF OUTPUT ==="
cat "$OUTPUT_DIFF"
echo ""

# Count provenance types
base_collision_count=$(grep -c "NAC-Merge-Result: new-content" "$OUTPUT_BASE" || echo 0)
diff_collision_count=$(grep -c "NAC-Merge-Result: new-content" "$OUTPUT_DIFF" || echo 0)
base_total=$(grep -c "^WARC/1.0$" "$OUTPUT_BASE" || echo 0)
diff_total=$(grep -c "^WARC/1.0$" "$OUTPUT_DIFF" || echo 0)

echo "Base output: $base_total records ($base_collision_count collision)"
echo "Diff output: $diff_total records ($diff_collision_count collision)"

# Verify expectations
# Collision: Both records should be emitted to both outputs with "new-content" provenance
EXPECTED_BASE=2
EXPECTED_DIFF=2
EXPECTED_COLLISION=2

if [[ $base_total -eq $EXPECTED_BASE ]] && \
   [[ $diff_total -eq $EXPECTED_DIFF ]] && \
   [[ $base_collision_count -eq $EXPECTED_COLLISION ]]; then
  echo "✓ Test passed: Collision handling correct"
  rm -rf "$OUTPUT_DIR"
  exit 0
else
  echo "✗ Test failed: Expected Base=$EXPECTED_BASE (collision=$EXPECTED_COLLISION), Diff=$EXPECTED_DIFF"
  echo "  Got: Base=$base_total (collision=$base_collision_count), Diff=$diff_total"
  exit 1
fi
