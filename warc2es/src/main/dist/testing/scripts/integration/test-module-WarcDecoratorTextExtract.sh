#!/usr/bin/env bash
# test-module-WarcDecoratorTextExtract.sh - Integration checks for text extraction path
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FIND_ROOT="$SCRIPT_DIR"
while [[ "$FIND_ROOT" != "/" && ! -f "$FIND_ROOT/pom.xml" ]]; do
  FIND_ROOT="$(dirname "$FIND_ROOT")"
done
PROJECT_ROOT="$FIND_ROOT"
mkdir -p "$PROJECT_ROOT/target/testing/tmp"

PIPELINE_BIN="${DIST_ROOT:-$PROJECT_ROOT/target/dist}/bin/warc-cli"
SHARED="$PROJECT_ROOT/shared"
[[ ! -e "$SHARED/example.com.warc.gz" ]] && SHARED="$PROJECT_ROOT/../shared"
INPUT="$SHARED/example.com.warc.gz"
OUT_DIR="$PROJECT_ROOT/target/testing/tmp/integration-textextract-$(date +%Y%m%d-%H%M%S)-$$"
mkdir -p "$OUT_DIR"

[[ -x "$PIPELINE_BIN" ]] || { echo "pipeline binary not found"; exit 1; }
[[ -f "$INPUT" ]] || { echo "input not found: $INPUT"; exit 1; }

run_case() {
  local name="$1"
  local prefix="$2"
  shift 2
  echo "[CASE] $name"
  "$PIPELINE_BIN" extract-text "$INPUT" --output-dir="$OUT_DIR" --output-prefix="$prefix" "$@" --silent
  mapfile -t outs < <(find "$OUT_DIR" -maxdepth 1 -type f -name "${prefix}-*.doet.gz" | LC_ALL=C sort)
  [[ ${#outs[@]} -gt 0 ]] || { echo "FAIL: $name (no output)"; exit 1; }
  zgrep -qi '^warc-type: conversion' "${outs[@]}" || { echo "FAIL: $name (no conversion records)"; exit 1; }
}

run_case "basic extraction" "basic"
run_case "min-length filter" "min50" --processor.extract-text.extract-min-text-length=50
run_case "lang filter en" "lang-en" --processor.lang-detect.lang-filter=en

echo "PASS: Text extraction integration checks"
