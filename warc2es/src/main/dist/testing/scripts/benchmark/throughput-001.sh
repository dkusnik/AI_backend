#!/usr/bin/env bash
# @name: throughput-001
# @group: benchmark
# @level: B1
# @timeout: 120s
# @keywords: benchmark, throughput, glacial
# @runs: 7

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

OUTPUT="$PROJECT_ROOT/target/testing/tmp/throughput-001-$$.gz"
trap "rm -f $OUTPUT" EXIT

# Measure throughput on 500MB sample
START_TIME=$(date +%s.%N)
# Use --benchmark --progress-none --final-report-summary as per CLI v2
"$CLI" warc-generic "$SHARED/bench-500m.warc.gz" --output="$OUTPUT" --benchmark --progress-none --final-report-summary > $PROJECT_ROOT/target/testing/tmp/bench-$$.log 2>&1
END_TIME=$(date +%s.%N)

ELAPSED=$(echo "$END_TIME - $START_TIME" | bc)
# Extract throughput: matches "31.71  MB/s (avg)" or "0 MB/s"
THROUGHPUT=$(grep -oP '[0-9.]+(?=[[:space:]]*MB/s)' $PROJECT_ROOT/target/testing/tmp/bench-$$.log | head -1 || echo "0")

echo "[throughput-001] OK elapsed=${ELAPSED}s throughput=${THROUGHPUT}MB/s"
rm -f $PROJECT_ROOT/target/testing/tmp/bench-$$.log
exit 0
