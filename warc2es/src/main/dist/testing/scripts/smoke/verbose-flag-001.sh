#!/usr/bin/env bash
# @name: verbose-flag-001
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

# Test --verbose flag with --dry-run
# Should contain detailed configuration dump
if "$CLI" warc2warc "$SHARED/example.com.warc.gz" --dry-run --verbose 2>&1 | grep -qiE "(EFFECTIVE CONFIGULATION|producer\\.)"; then
    echo "[verbose-flag-001] OK"
    exit 0
else
    echo "[verbose-flag-001] NOK: Expected verbose configuration dump"
    exit 1
fi
