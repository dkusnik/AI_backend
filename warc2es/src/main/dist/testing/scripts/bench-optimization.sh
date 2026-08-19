#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FIND_ROOT="$SCRIPT_DIR"
while [[ "$FIND_ROOT" != "/" && ! -f "$FIND_ROOT/pom.xml" ]]; do
  FIND_ROOT="$(dirname "$FIND_ROOT")"
done
PROJECT_ROOT="$FIND_ROOT"
mkdir -p "$PROJECT_ROOT/target/testing/tmp"

PIPELINE_BIN="${DIST_ROOT:-$PROJECT_ROOT/target/dist}/lib/scripts/pipeline-direct"

TMP_DIR="$PROJECT_ROOT/target/testing/tmp/bench-optimization"
mkdir -p "$TMP_DIR"
RESULTS="$TMP_DIR/results.csv"
rm -f "$RESULTS"

SHARED="$PROJECT_ROOT/shared"
[[ ! -e "$SHARED/example.com.warc.gz" ]] && SHARED="$PROJECT_ROOT/../shared"
INPUT_FILE="$SHARED/example.com.warc.gz"
[[ -f "$INPUT_FILE" ]] || INPUT_FILE="$PROJECT_ROOT/warc/input.warc.gz"

if [[ ! -x "$PIPELINE_BIN" ]]; then
  echo "SKIP: pipeline binary not found"
  exit 0
fi
if [[ ! -f "$INPUT_FILE" ]]; then
  echo "SKIP: benchmark input missing"
  exit 0
fi

echo "=== Benchmarking Optimization Campaign ==="
echo "Input: $INPUT_FILE"
echo "PIPELINE: $PIPELINE_BIN"

run_bench() {
  local name="$1"
  local mode="$2"
  local output="$3"
  local logfile="$TMP_DIR/${name}.log"
  rm -f "$output"

  local start end
  start=$(date +%s%N)
  case "$mode" in
    grep)
      "$PIPELINE_BIN" warc-generic --output="$output" "$INPUT_FILE" \
        --processor.grep.enabled=true --processor.grep.allow-warc-types=response --silent > "$logfile" 2>&1
      ;;
    extract)
      "$PIPELINE_BIN" warc-generic --output="$output" "$INPUT_FILE" \
        --processor.extract-text.enabled=true --silent > "$logfile" 2>&1
      ;;
    *)
      echo "Unknown mode: $mode" >&2
      return 1
      ;;
  esac
  end=$(date +%s%N)

  local ms=$(( (end - start) / 1000000 ))
  local size=0
  [[ -f "$output" ]] && size=$(du -m "$output" | cut -f1)
  echo "$name,$ms,$size" >> "$RESULTS"
  echo "OK: $name (${ms}ms, ${size}MB)"
}

run_bench "grep-response" "grep" "$TMP_DIR/bench_grep.warc.gz" || { echo "FAIL: grep-response"; exit 1; }
run_bench "extract-text" "extract" "$TMP_DIR/bench_extract.wet.gz" || { echo "FAIL: extract-text"; exit 1; }

echo "=== Results ==="
cat "$RESULTS"
