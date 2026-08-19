#!/usr/bin/env bash
# @name: config-positional-args-001
# @group: functional
# @level: L1
# @timeout: 10s
# @keywords: configuration, positional, mapping
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

# F-CFG-007: explicit output mapping
if ! "$CLI" warc2warc --output="$PROJECT_ROOT/target/testing/tmp/my-output.warc.gz" "$SHARED/example.com.warc.gz" --dry-run --verbose 2>&1 | grep -q "consumer.codec.file = $PROJECT_ROOT/target/testing/tmp/my-output.warc.gz"; then
    echo "[config-positional-args-001] F-CFG-007 FAIL: Explicit output file mapping error"
    exit 1
fi

# F-CFG-008: positional inputs mapping
OUTPUT=$("$CLI" warc2warc "in1.warc" "in2.warc" "in3.warc" --dry-run --verbose 2>&1)
if ! echo "$OUTPUT" | grep -q "producer.archive-chunked.files ="; then
    echo "[config-positional-args-001] F-CFG-008 FAIL: Input files mapping error"
    exit 1
fi
if ! echo "$OUTPUT" | grep -q "in1.warc" || ! echo "$OUTPUT" | grep -q "in2.warc" || ! echo "$OUTPUT" | grep -q "in3.warc"; then
    echo "[config-positional-args-001] F-CFG-008 FAIL: Some input files not mapped"
    exit 1
fi

echo "[config-positional-args-001] OK"
exit 0
