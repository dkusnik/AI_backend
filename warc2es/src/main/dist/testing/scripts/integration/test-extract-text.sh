#!/usr/bin/env bash
# test-extract-text.sh - extraction integration smoke
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
FIND_ROOT="$SCRIPT_DIR"
while [[ "$FIND_ROOT" != "/" && ! -f "$FIND_ROOT/pom.xml" ]]; do
  FIND_ROOT="$(dirname "$FIND_ROOT")"
done
PROJECT_ROOT="$FIND_ROOT"
mkdir -p "$PROJECT_ROOT/target/testing/tmp"

WARC_CLI="${DIST_ROOT:-$PROJECT_ROOT/target/dist}/bin/warc-cli"
SHARED="$PROJECT_ROOT/shared"
[[ ! -e "$SHARED/example.com.warc.gz" ]] && SHARED="$PROJECT_ROOT/../shared"
INPUT="$SHARED/example.com.warc.gz"

OUT_DIR="$PROJECT_ROOT/target/testing/tmp/wet-$(date +%Y%m%d-%H%M%S)-$$"
mkdir -p "$OUT_DIR"
PREFIX="test-extract"

[[ -f "$INPUT" ]] || { echo "input missing: $INPUT"; exit 1; }

echo "[TEST 1] Dry run"
"$WARC_CLI" extract-text "$INPUT" --output-dir="$OUT_DIR" --output-prefix="$PREFIX" --dry-run > /dev/null

echo "[TEST 2] Full extraction"
"$WARC_CLI" --profile=light extract-text "$INPUT" --output-dir="$OUT_DIR" --output-prefix="$PREFIX" --silent --progress-none --final-report-summary

mapfile -t OUTPUTS < <(find "$OUT_DIR" -maxdepth 1 -type f -name "${PREFIX}-*.doet.gz" | LC_ALL=C sort)
[[ ${#OUTPUTS[@]} -gt 0 ]] || { echo "FAIL: Output files missing"; exit 1; }

echo "[TEST 3] Output validation"
RECORD_COUNT=0
for output in "${OUTPUTS[@]}"; do
  zgrep -qi '^warc-type: conversion' "$output" || { echo "FAIL: no conversion records in $output"; exit 1; }
  RECORD_COUNT=$(( RECORD_COUNT + $(zgrep -ic '^warc/1' "$output" 2>/dev/null || echo 0) ))
done
echo "Output records: $RECORD_COUNT"
[[ "$RECORD_COUNT" -gt 0 ]] || { echo "FAIL: no records"; exit 1; }

echo "PASS: extract-text integration"
