#!/bin/bash
# bench-b1-b2-parallel.sh - Run B1 and B2 with the light-parallel profile (4 cores, 1GB)
# @timeout: 1200
# @runs: 1
#
# This tests the virtual-thread engine with the profile's parallel settings.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Set JVM options for 4-core parallel testing
# These override the defaults in pipeline-lib
export JAVA_OPTS="-Xms1g -Xmx1g -XX:+UseZGC -XX:ActiveProcessorCount=4"

# Apply the documented light-parallel engine settings directly. The distribution does not ship a
# config-light-parallel.yaml file, so WARC_CLI_PROFILE would silently fall back to the base config.
export BENCH_ENGINE_ARGS="--engine.concurrency=25 --engine.maxRecords=10 --engine.parallelGzip=true"

# Override scenarios to B1 and B2 (tmpfs tests - no I/O noise)
export BENCH_SCENARIOS="B1 B2"

echo "=== Parallel Test Configuration ==="
echo "JVM: 4 cores, 1GB heap, ZGC"
echo "Engine: 25 concurrent operations, 10-record queue floor, parallel GZIP"
echo ""

exec "$SCRIPT_DIR/bench-optimization.sh"
