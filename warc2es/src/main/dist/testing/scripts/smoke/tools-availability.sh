#!/usr/bin/env bash
# @name: tools-availability-001
# @group: smoke
# @level: L0
# @timeout: 5s
# @keywords: environment, tools, fast
# @runs: 1

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

check_tool() {
    local tool="$1"
    local version_cmd="${2:-$tool --version}"
    if command -v "$tool" >/dev/null 2>&1; then
        echo -n "[tools-availability-001] $tool: FOUND - "
        $version_cmd | head -n 1
        return 0
    else
        echo "[tools-availability-001] $tool: MISSING"
        return 1
    fi
}

ERRORS=0

echo "Checking required system tools..."
# Look for PROJECT_ROOT by finding pom.xml
FIND_ROOT="$SCRIPT_DIR"
while [[ "$FIND_ROOT" != "/" && ! -f "$FIND_ROOT/pom.xml" ]]; do
    FIND_ROOT="$(dirname "$FIND_ROOT")"
done
PROJECT_ROOT="$FIND_ROOT"

check_tool "java" "java --version" || ERRORS=$((ERRORS+1))
check_tool "gzip" || ERRORS=$((ERRORS+1))
check_tool "bc" "bc --version" || ERRORS=$((ERRORS+1))
check_tool "parallel" "parallel --version" || ERRORS=$((ERRORS+1))
check_tool "jq" "jq --version" || ERRORS=$((ERRORS+1))
check_tool "perl" "perl --version" || ERRORS=$((ERRORS+1))
check_tool "flock" "flock --version" || ERRORS=$((ERRORS+1))
check_tool "grep" "grep --version" || ERRORS=$((ERRORS+1))
check_tool "awk" "awk --version" || ERRORS=$((ERRORS+1))
check_tool "timeout" "timeout --version" || ERRORS=$((ERRORS+1))

if [[ $ERRORS -eq 0 ]]; then
    echo "[tools-availability-001] OK All tools found"
    exit 0
else
    echo "[tools-availability-001] NOK Missing $ERRORS tools"
    exit 1
fi
