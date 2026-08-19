#!/usr/bin/env bash
# @name: dry-run-inputs-outputs-001
# @group: functional
# @level: L1
# @timeout: 10s
# @keywords: dry-run, inputs, outputs, error
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
CLI="${DIST_ROOT:-$PROJECT_ROOT/target/dist}/lib/scripts/pipeline-direct"
SHARED="$PROJECT_ROOT/shared"
[[ ! -e "$SHARED/example.com.warc.gz" && ! -e "$SHARED/bench-500m.warc.gz" ]] && SHARED="$PROJECT_ROOT/../shared"

# F-DRY-001: dry-run baseline should produce effective config
if "$CLI" warc2warc "$SHARED/example.com.warc.gz" --dry-run 2>&1 | grep -q "EFFECTIVE CONFIGULATION"; then
    echo "[dry-run-inputs-outputs-001] F-DRY-001 OK"
else
    echo "[dry-run-inputs-outputs-001] F-DRY-001 FAIL: Expected dry-run config output"
    exit 1
fi

# F-DRY-002: dry-run with missing input should not crash parser
if "$CLI" warc2warc "$PROJECT_ROOT/target/testing/tmp/non-existent-$$" --dry-run 2>&1 | grep -qiE "(EFFECTIVE CONFIGULATION|Dry run completed)"; then
    echo "[dry-run-inputs-outputs-001] F-DRY-002 OK"
else
    echo "[dry-run-inputs-outputs-001] F-DRY-002 FAIL: Expected dry-run handling for missing input"
    exit 1
fi

# F-DRY-005: dry-run with existing output should still validate shape
EXISTING_FILE="$PROJECT_ROOT/target/testing/tmp/existing-output-$$.warc.gz"
touch "$EXISTING_FILE"
trap 'rm -f "$EXISTING_FILE"' EXIT
if "$CLI" warc2warc --output="$EXISTING_FILE" "$SHARED/example.com.warc.gz" --dry-run 2>&1 | grep -q "EFFECTIVE CONFIGULATION"; then
    echo "[dry-run-inputs-outputs-001] F-DRY-005 OK"
else
    echo "[dry-run-inputs-outputs-001] F-DRY-005 FAIL: Expected dry-run output with existing file"
    exit 1
fi

# F-DRY-006: dry-run-output-exists-force
if "$CLI" warc2warc --output="$EXISTING_FILE" "$SHARED/example.com.warc.gz" --dry-run --force 2>&1 | grep -q "EFFECTIVE CONFIGULATION"; then
    echo "[dry-run-inputs-outputs-001] F-DRY-006 OK"
else
    echo "[dry-run-inputs-outputs-001] F-DRY-006 FAIL: Expected success with --force"
    exit 1
fi

echo "[dry-run-inputs-outputs-001] OK"
exit 0
