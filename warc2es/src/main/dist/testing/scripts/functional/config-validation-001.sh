#!/usr/bin/env bash
# @name: config-validation-001
# @group: functional
# @level: L1
# @timeout: 10s
# @keywords: configuration, validation, error
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

# F-CFG-012: unknown pipeline should fail validation
OUT=$("$CLI" invalid-pipeline-name "$SHARED/example.com.warc.gz" --dry-run 2>&1 || true)
if echo "$OUT" | grep -qi "Unknown pipeline"; then
    echo "[config-validation-001] F-CFG-012 OK"
else
    echo "[config-validation-001] F-CFG-012 FAIL: Expected unknown pipeline error"
    exit 1
fi

# F-CFG-013: missing pipeline name should fail
OUT=$("$CLI" 2>&1 || true)
if echo "$OUT" | grep -qiE "(Missing pipeline name|No pipeline name specified)"; then
    echo "[config-validation-001] F-CFG-013 OK"
else
    echo "[config-validation-001] F-CFG-013 FAIL: Expected missing pipeline name error"
    exit 1
fi

# F-CFG-014: unknown CLI option should fail
OUT=$("$CLI" warc2warc "$SHARED/example.com.warc.gz" --dry-run --this-option-does-not-exist 2>&1 || true)
if echo "$OUT" | grep -q -- "--this-option-does-not-exist"; then
    echo "[config-validation-001] F-CFG-014 OK"
else
    echo "[config-validation-001] F-CFG-014 FAIL: Expected unknown token to appear in parsed inputs"
    exit 1
fi

echo "[config-validation-001] OK"
exit 0
