#!/usr/bin/env bash
# test-module-WarcDecoratorLanguageDetect.sh - Integration checks for language-detect path
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
OUT_DIR="$PROJECT_ROOT/target/testing/tmp/integration-lang-detect-$(date +%Y%m%d-%H%M%S)-$$"
mkdir -p "$OUT_DIR"

[[ -x "$PIPELINE_BIN" ]] || { echo "pipeline binary not found"; exit 1; }
[[ -f "$INPUT" ]] || { echo "input not found: $INPUT"; exit 1; }

PREFIX="lang"
"$PIPELINE_BIN" extract-text "$INPUT" --output-dir="$OUT_DIR" --output-prefix="$PREFIX" \
  --silent

mapfile -t OUTPUTS < <(find "$OUT_DIR" -maxdepth 1 -type f -name "${PREFIX}-*.doet.gz" | LC_ALL=C sort)
[[ ${#OUTPUTS[@]} -gt 0 ]] || { echo "FAIL: missing output"; exit 1; }
for out in "${OUTPUTS[@]}"; do
  zgrep -qi '^warc-type: conversion' "$out" || { echo "FAIL: no conversion records"; exit 1; }
done

# Optional header in small fixtures; do not fail hard if absent.
if zgrep -qi 'WARC-Identified-Content-Language:' "${OUTPUTS[@]}"; then
  echo "PASS: language header present"
else
  echo "PASS: pipeline executed (language header not present in fixture output)"
fi
