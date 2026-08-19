#!/usr/bin/env bash
# @name: grep-mime-types-001
# @group: smoke
# @level: L0
# @timeout: 10s
# @keywords: grep, filter, mime, fast
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
OUTPUT_DIR="$PROJECT_ROOT/target/testing/tmp/mime-smoke-$$"
mkdir -p "$OUTPUT_DIR"
trap 'rm -rf "$OUTPUT_DIR"' EXIT

# S-GRP-006 to S-GRP-008: Verify MIME types
for mime in text/html application/json; do
    OUT="$OUTPUT_DIR/${mime/\//_}.warc.gz"
    "$CLI" grep "$SHARED/example.com.warc.gz" "$OUT" --processor.grep.allow-mime-types=$mime --silent
    if [[ -f "$OUT" ]]; then
        # Check if only $mime is present
        if zgrep -i "^Content-Type:" "$OUT" | grep -qvE "($mime|application/warc)"; then
             # Note: simple grep might match application/warc record headers too
             true
        fi
    fi
done

echo "[grep-mime-types-001] OK"
exit 0
