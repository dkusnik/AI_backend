#!/bin/bash
# bench-process-sharding.sh
# Usage: ./bench-process-sharding.sh [input_warc.gz]
# Scenarios: 2x2 and 2x3 configurations

set -u
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../../../" && pwd)"
JAR_FILE="${DIST_ROOT:-$PROJECT_ROOT/target/dist}/lib/pipeline.jar"
RAMDISK_DIR="${RAMDISK_DIR:-$PROJECT_ROOT/target/testing/tmp/ramdisk}"
INPUT_FILE="${1:-$RAMDISK_DIR/plock.ap.gov.pl.warc.gz}"
OUTPUT_DIR="$PROJECT_ROOT/target/testing/tmp/bench-sharding"
mkdir -p "$OUTPUT_DIR"

run_scenario() {
    local num_procs=$1
    local cores_per_proc=$2
    local total_cores=$((num_procs * cores_per_proc))
    echo ">>> Scenario: $num_procs Processes x $cores_per_proc Cores"
    export JAVA_OPTS="-Xms1G -Xmx1G -XX:+UseZGC -XX:ActiveProcessorCount=$cores_per_proc"
    pids=""
    for ((i=1; i<=num_procs; i++)); do
        ( java $JAVA_OPTS --add-modules jdk.incubator.vector -jar "$JAR_FILE" extract-text-bench --profile="light-parallel" --engine.activeProcessorCount="$cores_per_proc" --engine.parallelGzip=true "$INPUT_FILE" "/dev/null" --verbose --final-report-summary > "$OUTPUT_DIR/proc_${total_cores}_${i}.log" 2>&1 ) &
        pids="$pids $!"
    done
    for pid in $pids; do wait $pid; done
    total_mb_s=0
    for ((i=1; i<=num_procs; i++)); do
        tp=$(grep "MB/s (avg)" "$OUTPUT_DIR/proc_${total_cores}_${i}.log" | tail -n 1 | awk '{print $1}')
        [ -n "$tp" ] && total_mb_s=$(echo "$total_mb_s + $tp" | bc)
    done
    echo "Result: Total Throughput = $total_mb_s MB/s"
}

run_scenario 2 2
run_scenario 2 3
