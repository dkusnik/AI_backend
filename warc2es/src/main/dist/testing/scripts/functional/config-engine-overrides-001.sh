#!/usr/bin/env bash
# @name: config-engine-overrides-001
# @group: functional
# @level: L1
# @timeout: 30s
# @keywords: configuration, overrides, engine
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

# F-CFG-001: override-engine-type-virtual
if ! "$CLI" info "$INPUT" --dry-run --engine=virtual 2>&1 | grep -q "VirtualThreadEngine"; then
    echo "[config-engine-overrides-001] F-CFG-001 FAIL: Expected VirtualThreadEngine"
    exit 1
fi

# F-CFG-002: removed reactive engine must fail loudly.
set +e
reactive_output=$("$CLI" info "$INPUT" --dry-run --engine=reactive 2>&1)
reactive_rc=$?
set -e
if [[ "$reactive_rc" -eq 0 ]] || ! grep -q "engine type 'reactive' has been removed" <<< "$reactive_output"; then
    echo "[config-engine-overrides-001] F-CFG-002 FAIL: Expected removed-reactive diagnostic"
    exit 1
fi

# F-CFG-003: override-concurrency-1
# Check for engine.concurrency in dot dump
if ! "$CLI" info "$INPUT" --dry-run --threads=1 --verbose 2>&1 | grep -q "engine.concurrency = 1"; then
    echo "[config-engine-overrides-001] F-CFG-003 FAIL: Expected engine.concurrency=1 in config"
    exit 1
fi

# F-CFG-004: override-concurrency-16
if ! "$CLI" info "$INPUT" --dry-run --threads=16 --verbose 2>&1 | grep -q "engine.concurrency = 16"; then
    echo "[config-engine-overrides-001] F-CFG-004 FAIL: Expected engine.concurrency=16 in config"
    exit 1
fi

# F-CFG-006: override-max-records
max_records_output=$("$CLI" info "$INPUT" --threads=1 --max-records=100 --verbose 2>&1)
if ! grep -q "engine.maxRecords = 100" <<< "$max_records_output"; then
    echo "[config-engine-overrides-001] F-CFG-006 FAIL: Expected engine.maxRecords=100 in config"
    exit 1
fi
if ! grep -q "Parallel mode: 1 workers, queue capacity 128" <<< "$max_records_output"; then
    echo "[config-engine-overrides-001] F-CFG-006 FAIL: Expected maxRecords=100 to derive queue capacity 128"
    exit 1
fi

echo "[config-engine-overrides-001] OK"
exit 0
