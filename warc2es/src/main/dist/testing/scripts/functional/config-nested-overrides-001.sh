#!/usr/bin/env bash
# @name: config-nested-overrides-001
# @group: functional
# @level: L1
# @timeout: 10s
# @keywords: configuration, nested, overrides
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

# F-CFG-009: nested-override-producer
if ! "$CLI" warc2warc "$SHARED/example.com.warc.gz" --dry-run --verbose --producer.archive-chunked.threads=2 2>&1 | grep -q "producer.archive-chunked.threads = 2"; then
    echo "[config-nested-overrides-001] F-CFG-009 FAIL: Producer nested override failed"
    exit 1
fi

# F-CFG-010: nested-override-processor
if ! "$CLI" warc-grep "$SHARED/example.com.warc.gz" --dry-run --verbose --processor.grep.row-limit=50 2>&1 | grep -q "processor.grep.row-limit = 50"; then
    echo "[config-nested-overrides-001] F-CFG-010 FAIL: Processor nested override failed"
    exit 1
fi

# F-CFG-011: nested-override-consumer
if ! "$CLI" warc2warc "$SHARED/example.com.warc.gz" --dry-run --verbose --consumer.codec.compress=none 2>&1 | grep -qi "consumer.codec.compress = none"; then
    echo "[config-nested-overrides-001] F-CFG-011 FAIL: Consumer nested override failed"
    exit 1
fi

echo "[config-nested-overrides-001] OK"
exit 0
