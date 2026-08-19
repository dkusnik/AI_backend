#!/bin/bash
# bench-b1-b2.sh - Run B1 and B2 optimization benchmarks
# @timeout: 1200
# @runs: 1

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export BENCH_SCENARIOS="B1 B2"
exec "$SCRIPT_DIR/bench-optimization.sh"
