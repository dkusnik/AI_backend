#!/usr/bin/env bash
# @name: dry-run-json-format-001
# @group: functional
# @level: L2
# @timeout: 10s
# @keywords: dry-run, json, format
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

# F-DRY-008: dry-run-config-dump-json
# Verify structured output (YAML/JSON)
OUTPUT=$("$CLI" warc2warc /dev/null "$SHARED/example.com.warc.gz" --dry-run --format=json 2>&1)
if echo "$OUTPUT" | grep -q "pipeline: warc2warc" && echo "$OUTPUT" | grep -q "producer:" && echo "$OUTPUT" | grep -q "processors:"; then
    echo "[dry-run-json-format-001] OK"
else
    echo "[dry-run-json-format-001] FAIL: Expected structured config dump"
    exit 1
fi

exit 0
