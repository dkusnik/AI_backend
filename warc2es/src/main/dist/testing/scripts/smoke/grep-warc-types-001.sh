#!/usr/bin/env bash
# @name: grep-warc-types-001
# @group: smoke
# @level: L0
# @timeout: 10s
# @keywords: grep, filter, warc-type, fast
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
OUTPUT_DIR="$PROJECT_ROOT/target/testing/tmp/grep-smoke-$$"
mkdir -p "$OUTPUT_DIR"
trap 'rm -rf "$OUTPUT_DIR"' EXIT

# S-GRP-001 to S-GRP-005: Verify various WARC types
for type in response request; do
    OUT="$OUTPUT_DIR/$type.warc.gz"
    "$CLI" grep "$SHARED/example.com.warc.gz" "$OUT" --processor.grep.allow-warc-types="$type" --silent
    if [[ ! -f "$OUT" ]]; then
        echo "[grep-warc-types-001] NOK: No output for type $type"
        exit 1
    fi
    # Allow warcinfo records, but all other records should match requested type.
    if ! zgrep -i "^WARC-Type: $type" "$OUT" > /dev/null; then
        echo "[grep-warc-types-001] NOK: Did not find requested type $type in $OUT"
        exit 1
    fi
    if zgrep -i "^WARC-Type:" "$OUT" | awk '{print tolower($2)}' | tr -d '\r' | grep -qvE "^(warcinfo|$type)$"; then
        echo "[grep-warc-types-001] NOK: Found unexpected WARC types in $OUT"
        exit 1
    fi
done

echo "[grep-warc-types-001] OK"
exit 0
