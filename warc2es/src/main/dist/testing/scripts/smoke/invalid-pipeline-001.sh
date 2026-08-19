#!/usr/bin/env bash
# @name: invalid-pipeline-001
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

# Expect Exit 1 on unknown pipeline
OUTPUT=$("$CLI" unknown-pipeline-name /dev/null "$SHARED/example.com.warc.gz" --dry-run 2>&1 || true)
if echo "$OUTPUT" | grep -q "Unknown pipeline: unknown-pipeline-name"; then
    echo "[invalid-pipeline-001] OK"
    exit 0
else
    echo "[invalid-pipeline-001] NOK: Expected 'Unknown pipeline: unknown-pipeline-name'"
    exit 1
fi
