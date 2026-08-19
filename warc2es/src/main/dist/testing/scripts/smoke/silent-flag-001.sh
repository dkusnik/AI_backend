#!/usr/bin/env bash
# @name: silent-flag-001
# @group: smoke
# @level: L0
# @timeout: 5s
# @keywords: configuration, fast
# @runs: 1

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Look for PROJECT_ROOT by finding pom.xml
FIND_ROOT="$SCRIPT_DIR"
while [[ "$FIND_ROOT" != "/" && ! -f "$FIND_ROOT/pom.xml" ]]; do
    FIND_ROOT="$(dirname "$FIND_ROOT")"
done
PROJECT_ROOT="$FIND_ROOT"
CLI="${DIST_ROOT:-$PROJECT_ROOT/target/dist}/lib/scripts/pipeline-direct"
SHARED="$PROJECT_ROOT/shared"
[[ ! -e "$SHARED/example.com.warc.gz" && ! -e "$SHARED/bench-500m.warc.gz" ]] && SHARED="$PROJECT_ROOT/../shared"

# Test --silent flag with --dry-run
# Should produce minimal to no output on stdout
OUTPUT=$("$CLI" warc2warc /dev/null "$SHARED/example.com.warc.gz" --dry-run --silent 2>&1)
if [[ -z "$OUTPUT" ]]; then
    echo "[silent-flag-001] OK"
    exit 0
else
    # Allow some very basic logs if they are forced, but verify it's much shorter than normal
    NORMAL_COUNT=$("$CLI" warc2warc /dev/null "$SHARED/example.com.warc.gz" --dry-run 2>&1 | wc -l)
    SILENT_COUNT=$(echo "$OUTPUT" | wc -l)
    if [[ "$SILENT_COUNT" -lt "$NORMAL_COUNT" ]]; then
        echo "[silent-flag-001] OK (silent_count=$SILENT_COUNT, normal_count=$NORMAL_COUNT)"
        exit 0
    else
        echo "[silent-flag-001] NOK: Expected less output (silent_count=$SILENT_COUNT, normal_count=$NORMAL_COUNT)"
        exit 1
    fi
fi
