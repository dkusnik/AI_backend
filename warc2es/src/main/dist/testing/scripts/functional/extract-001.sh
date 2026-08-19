#!/usr/bin/env bash
# @name: extract-001
# @group: functional
# @level: L2
# @timeout: 30s
# @keywords: extract, text, slow
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

OUTPUT_DIR="$PROJECT_ROOT/target/testing/tmp/extract-001-$$"
PREFIX="extract-001"
mkdir -p "$OUTPUT_DIR"
trap "rm -rf $OUTPUT_DIR" EXIT

# Extract text with min length filter
INPUT="$SHARED/bench-500m.warc.gz"
[[ ! -f "$INPUT" ]] && INPUT="$SHARED/example.com.warc.gz"
"$CLI" extract-text "$INPUT" --output-dir="$OUTPUT_DIR" --output-prefix="$PREFIX" \
    --processor.extract-text.extract-min-text-length=100 \
    --silent

RECORDS=0
while IFS= read -r f; do
    RECORDS=$(( RECORDS + $(zgrep -ic "^warc/1\\.[01]" "$f" 2>/dev/null || true) ))
done < <(find "$OUTPUT_DIR" -maxdepth 1 -type f -name "${PREFIX}-*.doet.gz" | LC_ALL=C sort)

if [[ "$RECORDS" -gt 0 ]]; then
    echo "[extract-001] OK records=$RECORDS"
    exit 0
else
    echo "[extract-001] NOK records=0"
    exit 1
fi
