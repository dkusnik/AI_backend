#!/usr/bin/env bash
# Test: URL migration - same digest, different URL (content moved)
# Expected behavior: Both URLs emitted with appropriate provenance

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."

# Setup
TEST_NAME="merge-synthetic-s5"
OUTPUT_DIR="/tmp/warc-test-output-$$/${TEST_NAME}"
mkdir -p "$OUTPUT_DIR"

INPUT_F1="${OUTPUT_DIR}/s5-crawl1.wet"
INPUT_F2="${OUTPUT_DIR}/s5-crawl2.wet"
OUTPUT_BASE_GLOBAL="${OUTPUT_DIR}/s5-merged-global-base.wet"
OUTPUT_DIFF_GLOBAL="${OUTPUT_DIR}/s5-merged-global-diff.wet"
OUTPUT_BASE_URL="${OUTPUT_DIR}/s5-merged-url-base.wet"
OUTPUT_DIFF_URL="${OUTPUT_DIR}/s5-merged-url-diff.wet"

# Generate F1 (crawl 1) - content at /old-location
cat > "$INPUT_F1" << 'EOF'
WARC/1.0
content-type: text/plain; charset=utf-8
warc-payload-digest: xxh128:aaaaaaaaaaaaaaaa
warc-target-uri: http://example.com/old-location
warc-date: 2026-01-01T10:00:00Z
warc-type: conversion
content-length: 34

Same content at original location

EOF

# Generate F2 (crawl 2) - SAME CONTENT at /new-location
cat > "$INPUT_F2" << 'EOF'
WARC/1.0
content-type: text/plain; charset=utf-8
warc-payload-digest: xxh128:aaaaaaaaaaaaaaaa
warc-target-uri: http://example.com/new-location
warc-date: 2026-02-01T10:00:00Z
warc-type: conversion
content-length: 34

Same content at original location

EOF

# Compress inputs
gzip -f "$INPUT_F1"
gzip -f "$INPUT_F2"
INPUT_F1="${INPUT_F1}.gz"
INPUT_F2="${INPUT_F2}.gz"

# Test 1: GLOBAL dedup (same content = deduplicate regardless of URL)
echo "Running merge with GLOBAL deduplication..."
bin/warc-cli merge \
  --input.files="$INPUT_F1" \
  --processor.doet-accumulator.secondary-input="$INPUT_F2" \
  --processor.doet-accumulator.primary-file-pattern="s5-crawl1" \
  --processor.doet-accumulator.baseline-date=2026-01-01T10:00:00Z \
  --processor.doet-accumulator.mode=incremental \
  --processor.doet-accumulator.deduplicate-scope=global \
  --output.file="$OUTPUT_BASE_GLOBAL" \
  --consumer.warc-base.diff-output="$OUTPUT_DIFF_GLOBAL" \
  --output.format=WARC_WET \
  --global.verbose=false

# Test 2: URL-scoped dedup (same content at different URL = keep both)
echo "Running merge with URL-scoped deduplication..."
bin/warc-cli merge \
  --input.files="$INPUT_F1" \
  --processor.doet-accumulator.secondary-input="$INPUT_F2" \
  --processor.doet-accumulator.primary-file-pattern="s5-crawl1" \
  --processor.doet-accumulator.baseline-date=2026-01-01T10:00:00Z \
  --processor.doet-accumulator.mode=incremental \
  --processor.doet-accumulator.deduplicate-scope=url \
  --output.file="$OUTPUT_BASE_URL" \
  --consumer.warc-base.diff-output="$OUTPUT_DIFF_URL" \
  --output.format=WARC_WET \
  --global.verbose=false

# Decompress outputs
for f in "$OUTPUT_BASE_GLOBAL" "$OUTPUT_DIFF_GLOBAL" "$OUTPUT_BASE_URL" "$OUTPUT_DIFF_URL"; do
  gunzip -f "$f"
done
OUTPUT_BASE_GLOBAL="${OUTPUT_BASE_GLOBAL%.gz}"
OUTPUT_DIFF_GLOBAL="${OUTPUT_DIFF_GLOBAL%.gz}"
OUTPUT_BASE_URL="${OUTPUT_BASE_URL%.gz}"
OUTPUT_DIFF_URL="${OUTPUT_DIFF_URL%.gz}"

# Analyze GLOBAL results
echo ""
echo "=== GLOBAL MODE: BASE OUTPUT ==="
cat "$OUTPUT_BASE_GLOBAL"
echo ""
echo "=== GLOBAL MODE: DIFF OUTPUT ==="
cat "$OUTPUT_DIFF_GLOBAL"

global_base_total=$(grep -c "^WARC/1.0$" "$OUTPUT_BASE_GLOBAL" || echo 0)
global_diff_total=$(grep -c "^WARC/1.0$" "$OUTPUT_DIFF_GLOBAL" || echo 0)
global_base_migrated=$(grep -c "NAC-Merge-Result: uri-changed" "$OUTPUT_BASE_GLOBAL" || echo 0)

echo ""
echo "Global mode: Base=$global_base_total, Diff=$global_diff_total (uri-changed=$global_base_migrated)"

# Analyze URL-scoped results
echo ""
echo "=== URL MODE: BASE OUTPUT ==="
cat "$OUTPUT_BASE_URL"
echo ""
echo "=== URL MODE: DIFF OUTPUT ==="
cat "$OUTPUT_DIFF_URL"

url_base_total=$(grep -c "^WARC/1.0$" "$OUTPUT_BASE_URL" || echo 0)
url_diff_total=$(grep -c "^WARC/1.0$" "$OUTPUT_DIFF_URL" || echo 0)
url_base_only=$(grep -c "NAC-Merge-Result: base-only" "$OUTPUT_BASE_URL" || echo 0)
url_new=$(grep -c "NAC-Merge-Result: new" "$OUTPUT_BASE_URL" || echo 0)

echo ""
echo "URL mode: Base=$url_base_total (base-only=$url_base_only, new=$url_new), Diff=$url_diff_total"

# Verify expectations
# GLOBAL: Content moved to new URL → 1 record in base (uri-changed), 1 in diff
# URL: Different URLs → 2 records in base (1 base-only + 1 new), 1 in diff (new)
EXPECTED_GLOBAL_BASE=1
EXPECTED_GLOBAL_DIFF=1
EXPECTED_URL_BASE=2
EXPECTED_URL_DIFF=1

if [[ $global_base_total -eq $EXPECTED_GLOBAL_BASE ]] && \
   [[ $global_diff_total -eq $EXPECTED_GLOBAL_DIFF ]] && \
   [[ $url_base_total -eq $EXPECTED_URL_BASE ]] && \
   [[ $url_diff_total -eq $EXPECTED_URL_DIFF ]]; then
  echo ""
  echo "✓ Test passed: URL migration handling correct"
  rm -rf "$OUTPUT_DIR"
  exit 0
else
  echo ""
  echo "✗ Test failed:"
  echo "  Global: Expected Base=$EXPECTED_GLOBAL_BASE, Diff=$EXPECTED_GLOBAL_DIFF"
  echo "          Got Base=$global_base_total, Diff=$global_diff_total"
  echo "  URL: Expected Base=$EXPECTED_URL_BASE, Diff=$EXPECTED_URL_DIFF"
  echo "       Got Base=$url_base_total, Diff=$url_diff_total"
  exit 1
fi
