#!/usr/bin/env bash
# @name: grep-001
# @group: functional
# @level: L1
# @timeout: 15s
# @keywords: grep, filter, slow
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

OUTPUT="$PROJECT_ROOT/target/testing/tmp/grep-001-$$.warc.gz"
trap "rm -f $OUTPUT" EXIT

# Filter only response records with 200 OK
"$CLI" grep "$SHARED/example.com.warc.gz" "$OUTPUT" \
    --processor.grep.allow-warc-types=response \
    --processor.grep.allow-http-codes=200 \
    --processor.grep.mode=deny-allow-drop

RECORDS=$(zcat "$OUTPUT" | grep -c "^WARC/1.0" || true)

if [[ "$RECORDS" -gt 0 ]]; then
    echo "[grep-001] OK records=$RECORDS"
    exit 0
else
    echo "[grep-001] NOK records=0"
    exit 1
fi
