#!/usr/bin/env bash
# test-module-WarcDecoratorDigest.sh - Integration checks for digest algorithm overrides
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
OUT_DIR="$PROJECT_ROOT/target/testing/tmp/integration-digest-$(date +%Y%m%d-%H%M%S)-$$"
mkdir -p "$OUT_DIR"

[[ -x "$PIPELINE_BIN" ]] || { echo "pipeline binary not found"; exit 1; }
[[ -f "$INPUT" ]] || { echo "input not found: $INPUT"; exit 1; }

ALGS=(sha256 sha512 blake3 xxh128 sha1)
for alg in "${ALGS[@]}"; do
  PREFIX="${alg}"
  echo "[CASE] digest algorithm: $alg"
  "$PIPELINE_BIN" extract-text "$INPUT" --output-dir="$OUT_DIR" --output-prefix="$PREFIX" \
    --processor.digest.algorithm="$alg" \
    --silent
  mapfile -t OUTPUTS < <(find "$OUT_DIR" -maxdepth 1 -type f -name "${PREFIX}-*.doet.gz" | LC_ALL=C sort)
  [[ ${#OUTPUTS[@]} -gt 0 ]] || { echo "FAIL: output missing for $alg"; exit 1; }
  zgrep -qi '^warc-type: conversion' "${OUTPUTS[@]}" || { echo "FAIL: no conversion records for $alg"; exit 1; }
done

echo "PASS: Digest decorator integration checks"
