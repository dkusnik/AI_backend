#!/usr/bin/env bash
# @name: version-flag-001
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

# Test global --version flag
if "$CLI" --version 2>&1 | grep -q "[0-9]\+\.[0-9]\+\.[0-9]\+"; then
    echo "[version-flag-001] OK"
    exit 0
else
    echo "[version-flag-001] NOK: Expected version string"
    exit 1
fi
