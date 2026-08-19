#!/usr/bin/env bash
# bench-plock-extraction.sh - extraction benchmark smoke
# @timeout: 600
# @runs: 1
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FIND_ROOT="$SCRIPT_DIR"
while [[ "$FIND_ROOT" != "/" && ! -f "$FIND_ROOT/pom.xml" ]]; do
  FIND_ROOT="$(dirname "$FIND_ROOT")"
done
PROJECT_ROOT="$FIND_ROOT"
mkdir -p "$PROJECT_ROOT/target/testing/tmp"

WARC_CLI="${DIST_ROOT:-$PROJECT_ROOT/target/dist}/bin/warc-cli"

SHARED="$PROJECT_ROOT/shared"
[[ ! -e "$SHARED/plock.ap.gov.pl.warc.gz" ]] && SHARED="$PROJECT_ROOT/../shared"
INPUT="$SHARED/plock.ap.gov.pl.warc.gz"

OUT_DIR="$PROJECT_ROOT/target/testing/tmp/bench-$(date +%Y%m%d-%H%M%S)-$$"
mkdir -p "$OUT_DIR"
OUTPUT="$OUT_DIR/plock.wet.gz"

if [[ ! -f "$INPUT" ]]; then
  echo "SKIP: benchmark input missing: $INPUT"
  exit 0
fi

echo "Input: $INPUT"
echo "Output: $OUTPUT"
"$WARC_CLI" extract-text "$INPUT" "$OUTPUT" --silent --progress-none --final-report-summary --benchmark
[[ -s "$OUTPUT" ]] || { echo "FAIL: output missing"; exit 1; }

echo "RECORD_COUNT=$(zgrep -c '^WARC/1' "$OUTPUT" 2>/dev/null || echo 0)"
