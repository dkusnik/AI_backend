#!/usr/bin/env bash
# @name: extract-smoke-001
# @group: smoke
# @level: L0
# @timeout: 15s
# @keywords: extract, text, wet, fast
# @runs: 1

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Look for PROJECT_ROOT by finding pom.xml
FIND_ROOT="$SCRIPT_DIR"
while [[ "$FIND_ROOT" != "/" && ! -f "$FIND_ROOT/pom.xml" ]]; do
    FIND_ROOT="$(dirname "$FIND_ROOT")"
done
PROJECT_ROOT="$FIND_ROOT"
mkdir -p "$PROJECT_ROOT/target/testing/tmp"
CLI="${DIST_ROOT:-$PROJECT_ROOT/target/dist}/bin/warc-cli"
SHARED="$PROJECT_ROOT/shared"
[[ ! -e "$SHARED/example.com.warc.gz" && ! -e "$SHARED/bench-500m.warc.gz" ]] && SHARED="$PROJECT_ROOT/../shared"
OUTPUT_DIR="$PROJECT_ROOT/target/testing/tmp/extract-smoke-$$"
PREFIX="extract-smoke"
mkdir -p "$OUTPUT_DIR"
trap 'rm -rf "$OUTPUT_DIR"' EXIT

# S-EXT-001 through S-EXT-006: Verify text extraction
"$CLI" extract-text "$SHARED/example.com.warc.gz" --output-dir="$OUTPUT_DIR" --output-prefix="$PREFIX" --silent
OUTPUT="$(find "$OUTPUT_DIR" -maxdepth 1 -type f -name "${PREFIX}-*.doet.gz" | head -n1)"

if [[ -z "$OUTPUT" || ! -f "$OUTPUT" ]]; then
    echo "[extract-smoke-001] NOK: No output file created"
    exit 1
fi

# Verify it's a valid WET file (checks for WET headers)
if ! zgrep -q "^WARC/1.0" "$OUTPUT" || ! zgrep -qi "^content-type: text/plain" "$OUTPUT"; then
    echo "[extract-smoke-001] NOK: Not a valid WET file or missing plain text records"
    exit 1
fi

# Verify metadata preserved (Target-URI)
if ! zgrep -qi "WARC-Target-URI: https://example.com/" "$OUTPUT" && ! zgrep -qi "WARC-Target-URI: http://example.com/" "$OUTPUT"; then
    echo "[extract-smoke-001] NOK: Metadata (Target-URI) not preserved"
    exit 1
fi

echo "[extract-smoke-001] OK"
exit 0
