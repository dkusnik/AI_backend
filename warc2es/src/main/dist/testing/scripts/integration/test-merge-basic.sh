#!/usr/bin/env bash
# test-merge-basic.sh - merge integration smoke
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
INPUT_WARC="$SHARED/example.com.warc.gz"
TMP_DIR="$PROJECT_ROOT/target/testing/tmp/merge-test-$(date +%Y%m%d-%H%M%S)-$$"
mkdir -p "$TMP_DIR"

BASE_WET="$TMP_DIR/base.wet.gz"
MERGED_BASE="$TMP_DIR/merged-base.doet.gz"
MERGED_DIFF="$TMP_DIR/merged-diff.doet.gz"

[[ -f "$INPUT_WARC" ]] || { echo "input missing: $INPUT_WARC"; exit 1; }

echo "Generating base WET..."
"$WARC_CLI" extract-text "$INPUT_WARC" "$BASE_WET" --silent
[[ -s "$BASE_WET" ]] || { echo "FAIL: base not generated"; exit 1; }

echo "Merging base with itself..."
"$WARC_CLI" merge --output-base="$MERGED_BASE" --output-diff="$MERGED_DIFF" "$BASE_WET" "$BASE_WET" --silent
[[ -s "$MERGED_BASE" ]] || { echo "FAIL: merged base missing"; exit 1; }
[[ -s "$MERGED_DIFF" ]] || { echo "FAIL: merged diff missing"; exit 1; }

COUNT_BASE=$(zgrep -ic '^warc-type: conversion' "$MERGED_BASE" 2>/dev/null || echo 0)
COUNT_DIFF=$(zgrep -ic '^warc-type: conversion' "$MERGED_DIFF" 2>/dev/null || echo 0)

echo "merged-base conversions: $COUNT_BASE"
echo "merged-diff conversions: $COUNT_DIFF"
[[ "$COUNT_BASE" -gt 0 ]] || { echo "FAIL: empty merged base"; exit 1; }

echo "PASS: merge integration"
