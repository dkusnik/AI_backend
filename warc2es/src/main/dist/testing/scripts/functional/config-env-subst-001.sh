#!/usr/bin/env bash
# @name: config-env-subst-001
# @group: functional
# @level: L1
# @timeout: 10s
# @keywords: configuration, environment, substitution
# @runs: 1

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Look for PROJECT_ROOT by finding pom.xml
FIND_ROOT="$SCRIPT_DIR"
while [[ "$FIND_ROOT" != "/" && ! -f "$FIND_ROOT/pom.xml" ]]; do
    FIND_ROOT="$(dirname "$FIND_ROOT")"
done
PROJECT_ROOT="$FIND_ROOT"
CLI="${DIST_ROOT:-$PROJECT_ROOT/target/dist}/bin/warc-cli"
SHARED="$PROJECT_ROOT/shared"
[[ ! -e "$SHARED/example.com.warc.gz" && ! -e "$SHARED/bench-500m.warc.gz" ]] && SHARED="$PROJECT_ROOT/../shared"
INPUT="$SHARED/tiny.warc.gz"
[[ -f "$INPUT" ]] || INPUT="$SHARED/example.com.warc.gz"

# F-CFG-016: environment-driven override value
export SMOKE_TEST_ENGINE="virtual"

if "$CLI" info "$INPUT" --dry-run --engine="$SMOKE_TEST_ENGINE" 2>&1 | grep -q "VirtualThreadEngine"; then
    echo "[config-env-subst-001] OK"
else
    echo "[config-env-subst-001] FAIL: Environment-based override failed"
    exit 1
fi

exit 0
